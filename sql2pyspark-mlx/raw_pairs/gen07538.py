from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter((F.col("location").eqNullSafe(F.lit("Bangkok"))) & F.col("budget").between(1345000, 1992000))
    .select("*", F.length("dept_name").alias("dept_name_length"))
)

result = (
    cleaned
    .rollup(
        F.when(F.col("dept_name_length") >= 84, "PREMIUM").otherwise("STANDARD").alias("dept_name_length_tier"),
        "dept_name",
    )
    .agg(
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.size(F.collect_set("location")).alias("num_locations_2"),
    )
    .orderBy(F.col("chiang_mai_events").desc_nulls_last())
)
