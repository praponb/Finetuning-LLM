from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy(F.month("hire_date")).orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w_grp = Window.partitionBy("level", "dept_id").orderBy("salary_bucket", "id")
w5 = Window.partitionBy("level", "dept_id").orderBy("prev_salary_bucket", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-07-03").cast("date")) & F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("salary") >= 128000, "High").when(F.col("salary") >= 71000, "Medium").otherwise("Low").alias("salary_band"),
        F.lower("level").alias("level_lower"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_clean"),
    )
)

enriched = (
    base
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "is_remote",
        "level",
        "level_lower",
        "level_clean",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched_employees = (
    latest
    .filter(~F.col("level").isin(["L4", "L5"]))
    .select(
        "id",
        "email",
        "dept_id",
        "is_remote",
        "level",
        "level_lower",
        "salary_bucket",
        "last_known_level",
        F.lag("salary_bucket").over(w_grp).alias("prev_salary_bucket"),
        F.lead("salary").over(w_grp).alias("next_salary"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

scored = (
    enriched_employees
    .select(
        "id",
        "dept_id",
        "is_remote",
        "level",
        "level_lower",
        "salary_bucket",
        "next_salary",
        F.min("next_salary").over(w5).alias("last3_min_next_salary"),
        F.avg("next_salary").over(w5).alias("last3_avg_next_salary"),
        F.sum("next_salary").over(w5).alias("last3_sum_next_salary"),
    )
)

result = (
    scored
    .groupBy(
        "level",
        F.when(F.col("last3_min_next_salary") >= 157000, "PREMIUM").otherwise("STANDARD").alias("last3_min_next_salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("last3_min_next_salary")).otherwise(0)).alias("l1_last3_min_next_salary"),
        F.sum(F.when(F.col("level") == "L4", F.col("next_salary")).otherwise(0)).alias("l4_next_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("last3_avg_next_salary", 0.9).alias("last3_avg_next_salary_p90"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
    )
    .orderBy(F.col("l4_next_salary").desc_nulls_last())
)
