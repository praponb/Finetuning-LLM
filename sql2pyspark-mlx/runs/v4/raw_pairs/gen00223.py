from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("sale_date", "id")

filtered = (
    df_store_sales
    .filter(F.month("sale_date").isin([7, 8, 12]))
    .withColumn("sale_date_period", F.date_trunc("week", "sale_date"))
)

result = (
    filtered
    .select(
        "*",
        F.avg("units").over(w).alias("units_avg_5_prior"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
