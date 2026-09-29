from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 2856000) & F.col("location").isin(["Tokyo", "Bangkok"]) & F.col("dept_name").isin(["HR", "Legal"]))
    .select(
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.when(F.col("location").isin(["New York", "London"]), "Group A").otherwise("Other").alias("location_group"),
    )
)
