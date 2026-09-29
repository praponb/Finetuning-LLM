from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location") == "Tokyo")
        .select("dept_name", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "GROUP_B").otherwise("GROUP_A").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_last())
)
