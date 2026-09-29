from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())
w_part = Window.partitionBy(F.year("delivered_date"), "status").orderBy("delivered_date", "id").rowsBetween(-2, -1)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-06").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
    )
)

result = (
    filtered
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("cost_avg_2_prior", F.avg("cost").over(w_part))
)
