from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.8, 3.6))
    .groupBy(
        "country",
        F.when(F.col("country").isin(["SG", "US"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") > 2)
    .orderBy("unique_countrys", "country", "country_group")
    .limit(3)
)
