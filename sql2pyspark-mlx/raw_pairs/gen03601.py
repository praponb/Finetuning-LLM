from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name")

enriched = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York"]))
    .select(
        "*",
        F.max("budget").over(w_part).alias("last4_max_budget"),
        F.count("budget").over(w_part).alias("last4_count_budget"),
        F.avg("budget").over(w_part).alias("last4_avg_budget"),
        F.sum("budget").over(w2).alias("location_sum_budget"),
    )
)

matched_keys = (
    enriched
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Engineering", "Sales", "Marketing"]))
        .select("dept_name", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") != "L1"), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([3, 21, 23, 25])), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.sum("location_sum_budget").over(w2).alias("location_sum_location_sum_budget"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNotNull()), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "last4_count_budget",
        "location_sum_budget",
        F.when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "OTHER_ZONE").when(F.col("dept_name").rlike("^(Sales|HR)$"), "PRIORITY").otherwise("EXTENDED").alias("dept_name_zone"),
    )
    .orderBy(F.col("budget").desc_nulls_last(), F.col("location_sum_budget").desc())
)
