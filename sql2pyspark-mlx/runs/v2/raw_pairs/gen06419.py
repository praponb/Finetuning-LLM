from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("budget").between(3523000, 3552000) & F.col("dept_name").isin(["Marketing", "HR"]))
)

agg = (
    src
    .groupBy(
        F.when(F.col("budget") >= 4190000, "high").when(F.col("budget") >= 990000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = agg.select("budget_bucket", "location_values")
