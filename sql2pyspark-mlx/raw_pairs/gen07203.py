from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy("row_num", "id")
w3 = Window.partitionBy("city", "country").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("rating")
w5 = Window.partitionBy("country").orderBy(F.col("next_rating").asc_nulls_last())
w6 = Window.partitionBy("country", "city").orderBy(F.col("rank_in_group").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("country").orderBy(F.col("next_rating_p95").desc())

cleaned = (
    df_suppliers
    .filter(~F.col("supplier_name").like("L%"))
    .select("*", F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .filter(F.col("row_num") <= 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "supplier_name_normalized",
        "row_num",
        F.lead("rating", 2).over(w2).alias("next_rating"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.percent_rank().over(w4).alias("rating_percent_rank"),
    )
)

ranked_suppliers = (
    scored
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked_suppliers
    .withColumn("row_num_2", F.row_number().over(w6))
    .filter(F.col("row_num_2") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.stock_qty"))
)

result = (
    latest_match
    .groupBy("country", F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating_percent_rank")).otherwise(0)).alias("berlin_rating_percent_rank"),
        F.round(F.avg("next_rating"), 2).alias("avg_next_rating"),
        F.percentile_approx("next_rating", 0.95).alias("next_rating_p95"),
    )
    .withColumn("next_rating_p95_rank", F.rank().over(w8))
    .orderBy(F.col("next_rating_p95").desc_nulls_last())
)
