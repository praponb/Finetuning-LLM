from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("age") == 63) & ~F.col("is_active") & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
)

segment_summary = (
    filtered_customers
    .groupBy("segment")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)

result = (
    segment_summary
    .filter(F.col("age_range") < 31)
    .select("segment", "age_range")
)
