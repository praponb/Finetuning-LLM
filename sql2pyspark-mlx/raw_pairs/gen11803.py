from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").isNotNull()), "left_anti")
)

scored = (
    not_flagged
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("next_budget", F.lead("budget", 1).over(w2))
    .withColumn("dept_name_max_budget", F.max("budget").over(w3))
)

cleaned_departments = (
    scored
    .filter(F.col("budget") <= 1622000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_title",
        "event_seq",
        "next_budget",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

result = (
    cleaned_departments
    .select(
        "id",
        "location",
        "budget",
        "location_title",
        "event_seq",
        "next_budget",
        "location_filled",
        "dept_name_clean",
        F.when(F.col("location").rlike("^(New York|Bangkok)$"), "EXTENDED").otherwise("CORE").alias("location_zone"),
    )
    .orderBy(F.col("next_budget").asc_nulls_first(), "location")
)
