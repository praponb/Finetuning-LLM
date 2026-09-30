from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("next_salary").desc_nulls_last())
w4 = Window.orderBy("last_known_prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_salary", "id")
w6 = Window.orderBy(F.col("cumulative_next_salary").desc())
w7 = Window.partitionBy("cumulative_next_salary_2_tier").orderBy(F.col("avg_next_salary_cume_dist").desc())

with_windows = (
    df_employees
    .select(
        "id",
        "email",
        "is_remote",
        F.lag("salary").over(w).alias("prev_salary"),
        F.lead("salary").over(w).alias("next_salary"),
        F.row_number().over(w).alias("row_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("row_seq").between(6, 7))
    .withColumn("clean_email", F.trim("email"))
)

scored = (
    filtered
    .filter(F.col("is_remote"))
    .select(
        "id",
        "email",
        "is_remote",
        "prev_salary",
        "next_salary",
        F.last("prev_salary", ignorenulls=True).over(w2).alias("last_known_prev_salary"),
        F.cume_dist().over(w3).alias("next_salary_cume_dist"),
        F.sum("next_salary").over(w2).alias("cumulative_next_salary"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "prev_salary",
        "last_known_prev_salary",
        "next_salary_cume_dist",
        F.sum("next_salary").over(w4).alias("cumulative_next_salary_2"),
        F.ntile(4).over(w5).alias("next_salary_bucket"),
        F.dense_rank().over(w6).alias("cumulative_next_salary_dense_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_next_salary_2") >= 611538, "PREMIUM").otherwise("STANDARD").alias("cumulative_next_salary_2_tier"),
        F.when(F.col("prev_salary") >= 162000, "LARGE").otherwise("SMALL").alias("prev_salary_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_next_salary_dense_rank", [0.25, 0.5, 0.75]).alias("cumulative_next_salary_dense_rank_quartiles"),
        F.round(F.avg("next_salary_cume_dist"), 2).alias("avg_next_salary_cume_dist"),
        F.percentile_approx("cumulative_next_salary_2", 0.5).alias("cumulative_next_salary_2_p50"),
    )
    .filter(F.col("avg_next_salary_cume_dist") > 0)
    .withColumn("avg_next_salary_cume_dist_rank", F.rank().over(w7))
    .orderBy(F.col("prev_salary_tier").desc_nulls_last())
)
