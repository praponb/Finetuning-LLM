from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "London"]) & ((F.col("location") == "London") | F.col("budget").between(2779000, 2864000)))
    .withColumn("location_dept_name", F.concat("location", F.lit(" "), "dept_name"))
)

departments_stats = (
    base
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
    )
)

result = (
    departments_stats
    .filter(F.col("pct_legal") > 25)
    .select("dept_name", "pct_legal")
    .orderBy("dept_name")
)
