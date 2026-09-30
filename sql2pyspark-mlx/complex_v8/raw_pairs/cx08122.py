from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_holder_joined = Window.partitionBy("holder_region", F.month("holder_joined_on")).orderBy(F.col("end_date_year").desc_nulls_first())
w_holder_region_product = Window.partitionBy("holder_region", "product")
w_holder_region_holder_joined_2 = Window.partitionBy("holder_region", F.month("holder_joined_on")).orderBy(F.col("end_date_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_policies
    .filter((~F.col("status").isin("EXPIRED", "ACTIVE")) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.year(F.col("end_date")).alias("end_date_year"),
        F.when(F.col("premium") >= 4670.9, "HIGH").when(F.col("premium") >= 2033.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

with_parent_holder = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") != "RETAIL"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date_year",
        "s.premium_filled",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

windowed = (
    with_parent_holder
    .select(
        "*",
        F.dense_rank().over(w_holder_region_holder_joined).alias("end_date_year_dense_rank"),
        F.count(F.lit(1)).over(w_holder_region_product).alias("rows_in_holder_region_product"),
        F.round(F.percent_rank().over(w_holder_region_holder_joined), 3).alias("end_date_year_percent_rank"),
        F.nth_value("end_date_year", 2).over(w_holder_region_holder_joined_2).alias("second_end_date_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "end_date_year",
        "rows_in_holder_region_product",
        "end_date_year_dense_rank",
    )
    .unpivot(
        ids=["id"],
        values=["end_date_year", "rows_in_holder_region_product", "end_date_year_dense_rank"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
