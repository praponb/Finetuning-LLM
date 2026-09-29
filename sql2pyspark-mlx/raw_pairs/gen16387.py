from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("stock_qty") >= 332, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_per_acme"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
    )
    .filter(F.col("cost_per_acme") >= 0)
)
