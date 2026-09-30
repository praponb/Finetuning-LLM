from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy(F.col("weight_kg").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("category_group", "category").orderBy(F.col("move_count").desc(), F.col("id").desc())

prepared = (
    df_skus
    .filter(F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
        F.when(F.col("category").isin("Toys", "Food"), "group_a").when(F.col("category") == "Beauty", "group_b").otherwise("other").alias("category_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    prepared.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

latest_per_key = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_per_key_2 = (
    latest_per_key
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key_2
    .groupBy("category_group", "move_count")
    .pivot("category", ["Food", "Apparel"])
    .agg(F.sum("unit_cost_per_weight_kg"))
    .orderBy("category_group", "move_count")
)
