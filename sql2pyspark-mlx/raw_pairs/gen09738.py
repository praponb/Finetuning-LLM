from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id")

metrics = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Singapore"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.lag("rating", 2).over(w2).alias("prev_rating"),
        F.first("rating", ignorenulls=True).over(w).alias("first_known_rating"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_rating") >= 4.2, "PREMIUM").otherwise("STANDARD").alias("prev_rating_tier"),
        "city",
        "country",
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("country") == "DE", F.col("cumulative_rating")).otherwise(0)).alias("de_cumulative_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("cumulative_rating")).otherwise(0)).alias("chiang_mai_cumulative_rating"),
    )
)
