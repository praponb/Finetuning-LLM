from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-04"))) & (F.col("shipped_date") < "2023-07-01"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "carrier")
)
