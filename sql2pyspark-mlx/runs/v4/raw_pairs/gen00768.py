from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget") <= 3169000)
    .select("*", F.upper("dept_name").alias("dept_name_upper"))
)

matched_keys = (
    valid
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name") == "Sales")
        .select("id", "location")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

valid_departments = (
    verified
    .filter(F.col("budget") > 2158000)
    .select(
        "id",
        "dept_name",
        "dept_name_upper",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

base = (
    valid_departments
    .filter(F.col("budget_int") == 70)
    .select(
        "id",
        "dept_name",
        "dept_name_upper",
        "budget_int",
        F.round("budget_int", 0).alias("budget_int_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        "dept_name",
        "dept_name_upper",
        "budget_int",
        F.when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy("dept_name")
)
