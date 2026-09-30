from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_product = Window.partitionBy("holder_region", "product")
w_all_by_end_date = Window.orderBy("end_date", "id")

valid = (
    df_policies
    .filter((F.col("product").isin("HEALTH", "LIFE")) & (F.col("start_date") < F.lit("2025-05-30").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.add_months(F.col("end_date"), 12).alias("end_date_plus_12m"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

windowed = (
    enriched
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_region_product), F.lit(0)), 4).alias("premium_share_of_holder_region_product"),
        F.round(F.avg("premium").over(w_holder_region_product), 2).alias("holder_region_product_avg_premium"),
        F.size(F.collect_set("holder_id").over(w_holder_region_product)).alias("distinct_holder_id_in_holder_region_product"),
        F.sum("premium").over(w_holder_region_product).alias("holder_region_product_sum_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_region_product), 2).alias("premium_vs_holder_region_product_avg"),
        F.row_number().over(w_all_by_end_date).alias("end_date_seq"),
    )
)

top_per_group = windowed.filter(F.col("end_date_seq") <= 5)

result = (
    top_per_group
    .select(
        "id",
        "premium_share_of_holder_region_product",
        "holder_region_product_avg_premium",
    )
    .unpivot(
        ids=["id"],
        values=["premium_share_of_holder_region_product", "holder_region_product_avg_premium"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
