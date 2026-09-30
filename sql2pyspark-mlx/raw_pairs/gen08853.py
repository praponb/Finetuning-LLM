from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").desc_nulls_last())
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

latest = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2024-10-23", "2025-06-01")), "left_semi")
)

scored = (
    kept
    .select(
        "*",
        F.sum("rating").over(w2).alias("running_rating"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
    )
)

latest_suppliers = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "running_rating", "rating_dense_rank")
)

result = (
    latest_suppliers
    .groupBy(
        F.when(F.col("running_rating") >= 20, "PREMIUM").otherwise("STANDARD").alias("running_rating_tier"),
        F.when(F.col("rating_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("rating_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
    )
)
