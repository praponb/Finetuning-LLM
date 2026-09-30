from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location")
w3 = Window.orderBy(F.col("budget").asc_nulls_first())

remaining_keys = (
    df_departments
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("location").isin(["Singapore", "Berlin", "Bangkok"]))
        .select("id", "location")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(~F.col("dept_name").isin(["Legal", "Sales"]))
    .withColumn("budget_level", F.when(F.col("budget") >= 3040000, "High").when(F.col("budget") >= 1522000, "Medium").otherwise("Low"))
)

matched_keys = (
    filtered
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("location") != "Berlin")
        .select("id", "dept_name")
    )
)

verified_departments = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

scored = verified_departments.withColumn("dept_name_count_budget", F.count("budget").over(w))

enriched = (
    scored
    .select(
        "*",
        F.max("dept_name_count_budget").over(w2).alias("location_max_dept_name_count_budget"),
        F.round((F.col("dept_name_count_budget") / F.sum("dept_name_count_budget").over(w)), 4).alias("dept_name_count_budget_share"),
        F.rank().over(w3).alias("budget_rank"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("location").isNotNull())
    .select(
        "dept_name",
        "location",
        "budget",
        "budget_level",
        "dept_name_count_budget",
        "dept_name_count_budget_share",
        "budget_rank",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

result = (
    cleaned
    .select(
        "dept_name",
        "location",
        "budget",
        "budget_level",
        "dept_name_count_budget",
        "budget_int",
        "location_filled",
    )
    .orderBy(F.col("budget").asc_nulls_first())
)
