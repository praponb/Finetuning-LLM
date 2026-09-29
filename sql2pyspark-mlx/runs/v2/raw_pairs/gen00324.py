from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.year("opened_date") >= 2023) & F.col("opened_date").between("2023-06-01", "2023-07-22"))
    .withColumn("city_length", F.length("city"))
)

summary = (
    src
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("city_length") >= 78, "high").when(F.col("city_length") >= 16, "mid").otherwise("low").alias("city_length_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = summary.orderBy(F.col("opened_month").desc())
