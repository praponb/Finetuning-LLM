from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2025-06-01")
    .groupBy(
        "status",
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .filter(F.col("min_cost") > 51)
)
