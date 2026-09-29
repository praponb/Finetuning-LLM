from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date_plus_62d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-09-17")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.date_add("opened_date", 62).alias("opened_date_plus_62d"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

result = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
)
