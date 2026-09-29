from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 39)
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
        F.year("delivered_date").alias("delivered_year"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("kerry_count") > 9)
    .orderBy("delivered_year")
)
