from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("supplier_name").like("Li%"))

country_summary = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
    )
)

result = country_summary.filter(F.col("th_rating") < 59)
