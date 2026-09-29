from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_scaled").desc(), "id")

filtered = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("New York")))
    .withColumn("location_upper", F.upper("location"))
    .withColumn("budget_scaled", F.round((F.col("budget") * 1.07), 2))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

deduped = (
    latest_match
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

daily = (
    deduped
    .groupBy(
        F.when(F.col("salary") >= 163000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("location") == "Chiang Mai", F.col("budget_scaled"))).alias("chiang_mai_avg_budget_scaled"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget_scaled")).otherwise(0)).alias("sales_budget_scaled"),
    )
)

result = (
    daily
    .select(
        "salary_tier",
        "num_rows",
        "chiang_mai_avg_budget_scaled",
        "sales_budget_scaled",
    )
)
