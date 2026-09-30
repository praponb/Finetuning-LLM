from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "SG") & F.col("supplier_name").like("%Pacifi%") & (F.col("country") != "DE"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("avg_rating").desc())
)
