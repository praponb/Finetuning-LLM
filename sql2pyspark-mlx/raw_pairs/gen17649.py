from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")
w2 = Window.partitionBy(F.year("delivered_date"), "carrier").orderBy(F.col("carrier_length").desc_nulls_last())
w3 = Window.orderBy("carrier_length", "id").rowsBetween(-2, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "delivered_date", "cost")
)

filtered = (
    ranked
    .filter((F.year("delivered_date") >= 2024) & F.col("carrier").isNotNull())
    .withColumn("cost_int", F.col("cost").cast("int"))
    .withColumn("carrier_length", F.length("carrier"))
)

enriched = (
    filtered
    .select(
        "id",
        "delivered_date",
        "cost_int",
        "carrier_length",
        F.cume_dist().over(w2).alias("carrier_length_cume_dist"),
        F.avg("carrier_length").over(w3).alias("carrier_length_avg_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("delivered_date") > F.lit("2023-06-01").cast("date"))
    .withColumn("carrier_length_whole", F.floor("carrier_length"))
    .withColumn("carrier_length_str", F.col("carrier_length").cast("string"))
)

result = (
    cleaned
    .select(
        "id",
        "cost_int",
        "carrier_length",
        "carrier_length_cume_dist",
        "carrier_length_avg_2",
        "carrier_length_str",
    )
)
