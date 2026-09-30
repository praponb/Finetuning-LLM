from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.lower(F.trim("name")) == "emma brown")
        .select("id", "city")
    )
)
