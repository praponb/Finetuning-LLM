from pyspark.sql import functions as F

src = df_departments.filter(F.col("location").isNotNull() & (F.col("budget") >= 2673000))

agg = (
    src
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = agg.orderBy("dept_name", "location_values")
