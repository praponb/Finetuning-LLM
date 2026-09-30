from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-02-28").cast("date"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 197, "High").when(F.col("quantity") >= 84, "Medium").otherwise("Low"))
)

with_windows = (
    cleaned
    .filter(F.col("store_id").isin([3, 4]))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("quantity_bucket", F.ntile(4).over(w2))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w3))
)

base = (
    with_windows
    .filter((F.col("last_restock") > F.lit("2023-06-19").cast("date")) & (F.col("row_seq") > 8))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "quantity_tier",
        "row_seq",
        "quantity_bucket",
        "next_quantity",
        (F.col("row_seq") * F.col("quantity")).alias("row_seq_times_quantity"),
        (F.datediff(F.current_date(), "last_restock") / 30).alias("months_since_last_restock"),
    )
)

matched_keys = (
    base
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([3, 4, 5, 6]))
        .select("id", "store_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("month", "last_restock").alias("activity_month"),
        F.date_trunc("year", "last_restock").alias("year_start"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("cnt"),
    )
)
