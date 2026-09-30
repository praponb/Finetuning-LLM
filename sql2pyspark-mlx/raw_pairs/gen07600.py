from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Acme"), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.6, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.round(F.avg("rating"), 2).alias("avg_rating_2"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("rating_per_london"),
    )
)
