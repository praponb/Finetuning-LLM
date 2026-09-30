from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc_nulls_last())

ranked = (
    df_employees
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("salary") >= 119000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Analyst", 1)), F.lit(0)), 2).alias("salary_per_analyst"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("salary_p50").asc_nulls_first())
)
