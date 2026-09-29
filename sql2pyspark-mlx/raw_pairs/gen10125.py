from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
    .groupBy("customer_id")
    .agg(
        F.countDistinct("country").alias("unique_countrys"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.unique_countrys") > 7)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("cs.unique_countrys"),
        F.col("cs.browser_list"),
    )
    .orderBy("id")
)
