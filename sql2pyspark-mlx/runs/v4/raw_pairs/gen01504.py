from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w_part = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("revenue_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("units").desc())

enriched = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("units_avg_5_prior"),
        F.ntile(3).over(w_part).alias("revenue_bucket"),
        F.avg("revenue").over(w3).alias("revenue_avg_3_prior"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.last("revenue_avg_3_prior", ignorenulls=True).over(w4).alias("last_known_revenue_avg_3_prior"),
        F.first("revenue_bucket", ignorenulls=True).over(w_grp).alias("first_known_revenue_bucket"),
        F.rank().over(w6).alias("units_rank"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.launch_date"), F.col("p.product_name"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("revenue_bucket") >= 7, "LARGE").otherwise("SMALL").alias("revenue_bucket_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_and("units").alias("units_bit_and"),
    )
)
