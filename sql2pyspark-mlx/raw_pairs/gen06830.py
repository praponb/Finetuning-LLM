from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("quarter_start").orderBy(F.col("distinct_levels").desc())

ranked = (
    df_employees
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .withColumn("distinct_levels_percent_rank", F.percent_rank().over(w2))
)
