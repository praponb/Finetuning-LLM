from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "department_location")

valid = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("level").eqNullSafe(F.lit("L4"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        F.concat("email", F.lit(" "), "level").alias("email_level"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.is_remote",
        "a.email_level",
        "a.resolved_level",
        F.col("b.budget"),
        F.col("b.location"),
        F.col("b.dept_name"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.is_remote",
        "src.location",
        "src.dept_name",
        F.col("b.location").alias("department_location"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("department_location").isNotNull())
    .withColumn("is_chiang_mai", F.when(F.col("department_location") == "Chiang Mai", 1).otherwise(0))
)

kept_keys = (
    cleaned
    .select("dept_id", "id")
    .exceptAll(
        df_employees
        .filter(F.lower(F.trim("email")) == "david.lee@gmail.com")
        .select("dept_id", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.dept_id",
        "a.location",
        "a.department_location",
        "a.is_chiang_mai",
        F.col("p.budget"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

result = (
    joined
    .filter(F.col("department_dept_name") == "Sales")
    .select("*", F.count("is_chiang_mai").over(w).alias("location_count_is_chiang_mai"))
)
