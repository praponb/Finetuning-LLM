from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating_filled").desc_nulls_first())
w_running = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("prev_rating_filled_filled").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Tokyo", "New York"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.when(F.col("city").isin(["Tokyo", "Chiang Mai"]), "Priority").otherwise("Other").alias("city_group"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .filter(F.col("rating_filled").between(25, 28))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating_filled",
        "city_clean",
        F.lag("rating_filled").over(w_running).alias("prev_rating_filled"),
        F.lead("rating_filled").over(w_running).alias("next_rating_filled"),
        F.lag("rating").over(w_running).alias("prev_rating"),
        F.lead("rating").over(w_running).alias("next_rating"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.city_clean",
        "s.prev_rating_filled",
        "s.next_rating_filled",
        "s.next_rating",
        F.col("c.category").alias("product_category"),
    )
)

base = (
    latest_match
    .filter(~F.col("product_category").eqNullSafe(F.lit("Grocery")))
    .select(
        "*",
        F.coalesce("prev_rating_filled", F.lit(0)).alias("prev_rating_filled_filled"),
        F.coalesce("product_category", F.lit("NONE")).alias("product_category_clean"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("product_category")
    .pivot("city", ["New York", "Bangkok"])
    .agg(
        F.sum("prev_rating_filled_filled").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "product_category",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_cnt").alias("new_york_cnt"),
        F.col("Bangkok_amount").alias("bangkok_amount"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
    )
)
