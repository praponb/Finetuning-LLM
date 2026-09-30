from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

result = (
    with_last
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
    )
    .filter(F.col("corr_stock_qty_cost") < 0)
    .orderBy(F.col("stock_qty_bit_and").desc())
)
