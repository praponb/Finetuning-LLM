from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "id",
        "country",
        F.lead("rating").over(w).alias("next_rating"),
        F.lag("rating").over(w).alias("prev_rating"),
        F.row_number().over(w).alias("row_seq"),
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("prev_rating") >= 4.0, "LARGE").otherwise("SMALL").alias("prev_rating_tier"),
        F.when(F.col("next_rating") >= 4.5, "LARGE").otherwise("SMALL").alias("next_rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.sum(F.when(F.col("country") == "US", F.col("prev_rating"))).alias("us_total_prev_rating"),
        F.avg("first_known_rating").alias("avg_first_known_rating"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("us_events") > 1)
    .orderBy("distinct_ids")
)
