from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating_adjusted", "id")
w2 = Window.partitionBy("country").orderBy("rating_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("prev_rating").desc())

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Impo%"))
    .withColumn("city_lower", F.lower("city"))
    .withColumn("rating_adjusted", F.round((F.col("rating") * 100), 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

metrics = (
    cleaned
    .filter(F.col("rating").isNotNull())
    .withColumn("prev_rating_adjusted", F.lag("rating_adjusted").over(w))
    .withColumn("prev_rating", F.lag("rating").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w2))
)

deduped = (
    metrics
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "supplier_name",
        "city",
        "rating_adjusted",
        "prev_rating_adjusted",
        "seq_no",
    )
)

base = (
    deduped
    .filter((~F.col("city").eqNullSafe(F.lit("London"))) & (F.col("seq_no") != 6))
    .select(
        "id",
        "supplier_name",
        "rating_adjusted",
        F.col("prev_rating_adjusted").cast("int").alias("prev_rating_adjusted_int"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("rating_adjusted") >= 60, "PREMIUM").otherwise("STANDARD").alias("rating_adjusted_tier"),
    )
    .agg(
        F.sum("rating_adjusted").alias("total_rating_adjusted"),
        F.max("prev_rating_adjusted_int").alias("max_prev_rating_adjusted_int"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").asc_nulls_last())
)
