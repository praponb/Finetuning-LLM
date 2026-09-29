from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]) & ((F.col("status") != "LOST") | (F.col("shipped_date") <= "2024-02-11")))
    .groupBy(F.year("shipped_date").alias("shipped_year"), "status")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.count("cost").alias("cost_count"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
    .filter(F.col("unique_statuss") < 1)
)
