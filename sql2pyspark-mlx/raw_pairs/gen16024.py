from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

ranked = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .filter(F.col("kerry_count") > 1)
    .orderBy(F.col("returned_cost").desc())
)
