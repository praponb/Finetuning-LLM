from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id")
w2 = Window.orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("revenue_share").asc_nulls_first(), "id")
w7 = Window.partitionBy("store_id")
w8 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").asc_nulls_first())

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-05-28")))
    .select("*", (F.col("revenue") - F.col("units")).alias("revenue_per_unit"))
)

enriched = (
    valid
    .withColumn("next_units", F.lead("units", 1, 0).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("next_units_2", F.lead("units", 1, 0).over(w3))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4))
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w5))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.avg("revenue").over(w7).alias("store_id_avg_revenue"),
        F.cume_dist().over(w8).alias("revenue_per_unit_cume_dist"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.price"), F.col("b.launch_date"))
)

result = (
    with_dims
    .withColumn("next_units_2_tier", F.when(F.col("next_units_2") >= 42, "PREMIUM").otherwise("STANDARD"))
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 4200, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["next_units_2_tier", "revenue_tier"], ["next_units_2_tier"]],
        "next_units_2_tier",
        "revenue_tier",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
    )
    .orderBy(F.col("distinct_store_ids").desc_nulls_last())
)
