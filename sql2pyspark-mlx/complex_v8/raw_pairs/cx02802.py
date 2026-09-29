from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_group = Window.partitionBy("holder_id", "product_group")
w_status_by_days_start_date_to = Window.partitionBy("status").orderBy("days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy("start_date", "id")
w_status_by_days_start_date_to_2 = Window.partitionBy("status").orderBy("days_start_date_to_end_date", "id")

cleaned_policys = (
    df_policies
    .filter((F.col("premium") >= 2111.3) & (F.col("end_date") >= F.lit("2025-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HEALTH", "HOME"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

screened = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "IN_REVIEW"), "left_anti")
    .filter(F.col("holder_id").isNotNull())
)

windowed = (
    screened
    .select(
        "*",
        F.sum("days_start_date_to_end_date").over(w_holder_id_product_group).alias("holder_id_product_group_sum_days_start_date_to_e"),
        F.last("days_start_date_to_end_date", ignorenulls=True).over(w_status_by_days_start_date_to).alias("last_known_days_start_date_to_end_date"),
        F.round(F.cume_dist().over(w_end_date_year_by_start_date), 3).alias("start_date_cume_dist"),
        F.lead("start_date", 2).over(w_status_by_days_start_date_to_2).alias("next2_start_date"),
        F.count(F.lit(1)).over(w_holder_id_product_group).alias("rows_in_holder_id_product_group"),
    )
)

with_details_holder = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.status_ref",
        "s.holder_id_product_group_sum_days_start_date_to_e",
        "s.start_date_cume_dist",
        "s.next2_start_date",
        "s.rows_in_holder_id_product_group",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_details_holder
    .select(
        "id",
        "rows_in_holder_id_product_group",
        "holder_id_product_group_sum_days_start_date_to_e",
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_holder_id_product_group", "holder_id_product_group_sum_days_start_date_to_e"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
