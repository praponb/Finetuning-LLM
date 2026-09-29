from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_plus_3m_year_by_s = Window.partitionBy(F.year("start_date_plus_3m")).orderBy(F.col("start_date_plus_3m").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_plus_3m_year_by_s_2 = Window.partitionBy(F.year("start_date_plus_3m")).orderBy(F.col("start_date_plus_3m").asc_nulls_last(), "id")
w_holder_region_status_by_star = Window.partitionBy("holder_region", "status").orderBy("start_date_plus_3m", "id")
w_holder_region_status_by_star_2 = Window.partitionBy("holder_region", "status").orderBy("start_date_plus_3m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.col("product") == "HEALTH") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.add_months(F.col("start_date"), 3).alias("start_date_plus_3m"),
        F.when(F.col("premium") >= 3521.8, "HIGH").when(F.col("premium") >= 1132.1, "MEDIUM").otherwise("LOW").alias("premium_tier"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

ranked = (
    prepared_policys
    .select(
        "id",
        "holder_id",
        "status",
        "start_date_plus_3m",
        "premium_tier",
        "end_date_dayofweek",
        F.sum("premium").over(w_start_date_plus_3m_year_by_s).alias("running_premium"),
        F.row_number().over(w_start_date_plus_3m_year_by_s_2).alias("seq_no"),
    )
)

with_details_holder = (
    ranked.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region") == "East")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.start_date_plus_3m",
        "s.end_date_dayofweek",
        "s.running_premium",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

metrics = (
    with_details_holder
    .select(
        "id",
        "holder_id",
        "status",
        "end_date_dayofweek",
        "running_premium",
        "holder_region",
        "holder_name",
        F.lead("running_premium").over(w_holder_region_status_by_star).alias("next_running_premium"),
        F.first("holder_risk_score", ignorenulls=True).over(w_holder_region_status_by_star_2).alias("first_known_holder_risk_score"),
    )
)

result = (
    metrics
    .select("id", "running_premium", "first_known_holder_risk_score")
    .unpivot(
        ids=["id"],
        values=["running_premium", "first_known_holder_risk_score"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
