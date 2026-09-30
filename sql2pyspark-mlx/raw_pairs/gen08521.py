from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.lower("supplier_name").like("%oky%"))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

country_group_summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("country").isin(["UK", "SG"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(F.countDistinct("rating").alias("unique_citys"))
)

result = (
    country_group_summary
    .filter(F.col("unique_citys") < 4)
    .select("country_group", "unique_citys")
    .orderBy("unique_citys", "country_group")
    .limit(1)
)
