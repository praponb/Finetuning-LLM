from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("quantity_scaled", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("city").orderBy("quantity_scaled", "id")
w3 = Window.partitionBy("city").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_scaled").asc_nulls_first(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 100), 2).alias("quantity_scaled"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.opened_date"), F.col("b.city"))
)

scored = (
    joined
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy("store_id")
    .pivot("city", ["Chiang Mai", "London"])
    .agg(F.avg("quantity").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "store_id",
        F.col("Chiang Mai_total").alias("chiang_mai_total"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
        F.col("London_total").alias("london_total"),
        F.col("London_n").alias("london_n"),
    )
)
