from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("rn", "id")
w3 = Window.partitionBy("dept_id", "job_title").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "job_title").orderBy("rn", "id")
w5 = Window.partitionBy("job_title").orderBy(F.col("prev_salary").desc_nulls_last(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-13").cast("date")) & (F.col("dept_id") == 37))
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.col("dept_id").isin([13, 33]))
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "clean_level",
        "rn",
        F.lag("salary", 1).over(w2).alias("prev_salary"),
        F.last("level", ignorenulls=True).over(w3).alias("last_known_level"),
    )
)

ranked_employees = (
    metrics
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
)

deduped = (
    ranked_employees
    .withColumn("rn_3", F.row_number().over(w5))
    .filter(F.col("rn_3") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("prev_salary") >= 130000, "LARGE").otherwise("SMALL").alias("prev_salary_tier"),
        F.when(F.col("salary") >= 121000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .orderBy(F.col("salary_tier").desc())
)
