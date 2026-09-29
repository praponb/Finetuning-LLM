from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("unit_price", "id").rowsBetween(-5, -1)

scored = (
    df_order_items
    .filter(F.col("order_id").isin([11, 14, 38, 39]))
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
    .withColumn("cumulative_quantity_2", F.sum("quantity").over(w_recent))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_recent))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w4))
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.percentile_approx("cumulative_quantity", 0.5).alias("cumulative_quantity_p50"),
        F.percentile_approx("first_known_quantity", 0.9).alias("first_known_quantity_p90"),
    )
    .orderBy(F.col("cumulative_quantity_p50").desc())
)
