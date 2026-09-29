from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("name") >= 6) & F.col("email").like("%@startup.io"))
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("stddev_credit_limit").desc())
)
