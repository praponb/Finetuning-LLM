from pyspark.sql import functions as F

dim_totals = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .cube(F.col("p.city").alias("city"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.amount")).alias("min_amount"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select(
        "status",
        "min_amount",
        "gid",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "GROUP_B").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
    )
)
