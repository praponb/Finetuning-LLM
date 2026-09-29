from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["New York", "London"]))
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(~F.col("city").isin(["New York", "London", "Berlin"]))
        .select("country", "city")
    )
)
