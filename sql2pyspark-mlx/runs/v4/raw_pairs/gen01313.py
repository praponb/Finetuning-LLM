from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("store_id").orderBy("row_num", "id")
w4 = Window.orderBy("last_restock", "id")
w5 = Window.partitionBy("store_id").orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("store_id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

ranked = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w_hist))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w5))
    .withColumn("store_id_count_quantity", F.count("quantity").over(w_running))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.launch_date"), F.col("p.brand"))
)

result = (
    with_dims
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "row_num",
        "quantity_avg_6_prior",
        "next_quantity",
        "row_seq",
        "quantity_avg_6",
        "store_id_count_quantity",
        "launch_date",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("quantity_avg_6").desc_nulls_last())
)
