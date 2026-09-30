from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("rn", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy("country", "city").orderBy("rn", "id")

latest = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.avg("rating").over(w2).alias("rating_avg_2_prior"),
        F.avg("rating").over(w3).alias("rating_avg_6"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

ranked = (
    metrics
    .withColumn("rn_2", F.row_number().over(w5))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

eligible = (
    ranked
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("Ba%")), "left_anti")
)

result = (
    eligible
    .groupBy("city")
    .pivot("country", ["US", "TH"])
    .agg(F.sum("seq_no").alias("total"), F.count(F.lit(1)).alias("n"))
)
