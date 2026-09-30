from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.count("*").alias("num_shipments"))
    .orderBy("num_shipments", F.col("delivered_cohort").desc())
)
