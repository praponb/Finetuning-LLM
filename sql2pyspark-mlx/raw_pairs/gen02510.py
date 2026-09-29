from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("category").orderBy("stock_qty_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("running_avg_price", "id")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy("price", "id")
w5 = Window.partitionBy("brand").orderBy(F.col("price").asc_nulls_last(), "id")

valid = (
    df_products
    .filter(~F.col("product_name").like("%USB%") & F.col("brand").isNotNull())
    .select("*", F.floor("stock_qty").alias("stock_qty_whole"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").between(210, 270)), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        F.count("price").over(w_grp).alias("running_count_price"),
        F.sum("price").over(w_grp).alias("running_sum_price"),
        F.avg("price").over(w_grp).alias("running_avg_price"),
    )
)

scored = (
    enriched
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w3))
    .withColumn("brand_avg_running_sum_price", F.avg("running_sum_price").over(w3))
    .withColumn("next_running_count_price", F.lead("running_count_price", 1).over(w4))
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "running_count_price",
        "running_sum_price",
        "running_avg_price",
        "corr_cost_price",
        "brand_avg_running_sum_price",
    )
    .orderBy(F.col("id").desc_nulls_first(), "corr_cost_price")
)
