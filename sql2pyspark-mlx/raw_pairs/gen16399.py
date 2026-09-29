from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"), "brand").orderBy("stock_qty", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.orderBy(F.col("cost").desc(), "id")

metrics = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "supplier_id",
        F.avg("cost").over(w).alias("cost_avg_3_prior"),
        F.avg("price").over(w_ordered).alias("price_avg_3"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cost") >= 700, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "supplier_id",
    )
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("price_avg_3"))).alias("fashion_max_price_avg_3"),
        F.min("cost_avg_3_prior").alias("min_cost_avg_3_prior"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("fashion_max_price_avg_3") < 1000000)
)
