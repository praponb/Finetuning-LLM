from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.upper(F.trim("name")) == "CAROL DIAZ") & F.col("name").isNotNull())
    .select("name")
)
