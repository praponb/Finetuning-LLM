from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_last())
w_part = Window.partitionBy("country").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("row_seq").desc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.col("rating").between(3.2, 4.2))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_tier"),
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "eco")), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "country",
        "supplier_name_upper",
        "city_clean",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.last("rating_percent_rank", ignorenulls=True).over(w_part).alias("last_known_rating_percent_rank"),
        F.first("row_seq", ignorenulls=True).over(w4).alias("first_known_row_seq"),
        F.cume_dist().over(w5).alias("row_seq_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("first_known_row_seq") >= 9, "LARGE").otherwise("SMALL").alias("first_known_row_seq_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.sum(F.when(F.col("country") == "US", F.col("row_seq_cume_dist")).otherwise(0)).alias("us_row_seq_cume_dist"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.avg(F.when(F.col("country") == "UK", F.col("rating_percent_rank"))).alias("uk_avg_rating_percent_rank"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.avg(F.when(F.col("country") == "US", F.col("last_known_rating_percent_rank"))).alias("us_avg_last_known_rating_percent_rank"),
    )
)
