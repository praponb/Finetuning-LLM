from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("segment").isin(["Corporate", "SMB"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") > 43)
    .drop("max_age")
)
