from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("country")
w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w5 = Window.partitionBy("country", "city")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("country").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy(F.col("cost").desc(), "id")

enriched = (
    df_suppliers
    .withColumn("rating_avg_2_prior", F.avg("rating").over(w))
    .withColumn("country_max_rating", F.max("rating").over(w2))
    .withColumn("rating_avg_5", F.avg("rating").over(w_hist))
    .withColumn("rating_percent_rank", F.percent_rank().over(w4))
    .withColumn("country_stddev_samp_rating", F.stddev_samp("rating").over(w5))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") != "Home"), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("city").isNotNull())
    .select("*", F.trim("supplier_name").alias("clean_supplier_name"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.rating_avg_2_prior",
        "s.country_max_rating",
        "s.rating_avg_5",
        "s.clean_supplier_name",
        F.col("c.cost"),
    )
)

enriched_suppliers = (
    latest_match
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_avg_2_prior",
        "clean_supplier_name",
        "cost",
        F.first("rating_avg_5", ignorenulls=True).over(w7).alias("first_known_rating_avg_5"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

not_flagged_suppliers = (
    enriched_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNull()), "left_semi")
)

stats = (
    not_flagged_suppliers
    .groupBy("city")
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("rating_per_london"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
    )
)

result = (
    stats
    .select(
        "city",
        "rating_per_london",
        "bangkok_rows",
        F.when(F.col("city").rlike("^(London|Chiang Mai)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("rating_per_london").asc_nulls_first())
)
