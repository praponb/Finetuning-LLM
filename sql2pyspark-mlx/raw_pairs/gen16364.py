from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("budget").between(1414000, 3210000))
    .select(
        "*",
        F.floor("budget").alias("budget_whole"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id") == 30), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.full_name").like("%Olivi%")), "left_semi")
)

filtered = (
    kept
    .filter(F.col("location").isin(["New York", "Singapore", "Berlin"]))
    .select(
        "id",
        "location",
        "budget",
        "budget_whole",
        "dept_name_upper",
        F.length("location").alias("location_length"),
        F.round("budget_whole", 2).alias("budget_whole_rounded"),
    )
)

cleaned_departments = (
    filtered
    .filter(F.col("location").isNotNull() & (F.col("budget_whole") == 28))
    .select(
        "id",
        "location",
        "budget",
        "budget_whole",
        "dept_name_upper",
        "location_length",
        (F.col("budget_whole") * F.col("budget")).alias("budget_whole_times_budget"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

result = (
    cleaned_departments
    .select(
        "id",
        "location",
        F.col("budget_whole_times_budget").alias("BUDGET_WHOLE_TIMES_BUDGET"),
        F.col("budget").alias("BUDGET_VAL"),
    )
    .unpivot(
        ids=["id", "location"],
        values=["BUDGET_WHOLE_TIMES_BUDGET", "BUDGET_VAL"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
