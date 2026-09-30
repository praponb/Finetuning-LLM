from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id")

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .cube(F.date_trunc("MONTH", "launch_date").alias("activity_month"), "category")
    .agg(
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("stock_qty_per_initech"),
        F.countDistinct("category").alias("distinct_categories"),
    )
    .orderBy(F.col("stock_qty_per_initech").desc())
)
