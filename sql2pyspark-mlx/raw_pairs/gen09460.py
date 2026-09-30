from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("job_title", "level")
    .intersectAll(
        df_employees
        .filter(F.col("salary") < 74000)
        .select("job_title", "level")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("salary") >= 138000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
    )
    .orderBy(F.col("salary_per_intern").asc_nulls_last())
)
